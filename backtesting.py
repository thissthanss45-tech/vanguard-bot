from __future__ import annotations

import pandas as pd


def simple_backtest(close: pd.Series, rsi: pd.Series) -> dict:
    if close is None or rsi is None or len(close) < 60 or len(rsi) < 60:
        return {"trades": 0, "win_rate": 0.0, "total_return_pct": 0.0}

    in_position = False
    entry = 0.0
    pnl = []

    for idx in range(1, len(close)):
        price = float(close.iloc[idx])
        rsi_prev = float(rsi.iloc[idx - 1]) if pd.notna(rsi.iloc[idx - 1]) else 50.0

        if not in_position and rsi_prev < 30:
            in_position = True
            entry = price
            continue

        if in_position and rsi_prev > 55:
            pnl.append((price - entry) / entry)
            in_position = False

    if in_position:
        price = float(close.iloc[-1])
        pnl.append((price - entry) / entry)

    if not pnl:
        return {"trades": 0, "win_rate": 0.0, "total_return_pct": 0.0}

    wins = sum(1 for x in pnl if x > 0)
    total_return = sum(pnl) * 100
    return {
        "trades": len(pnl),
        "win_rate": round((wins / len(pnl)) * 100, 2),
        "total_return_pct": round(total_return, 2),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Enhanced multi-strategy backtest
# ──────────────────────────────────────────────────────────────────────────────

def _pnl_stats(pnl: list[float]) -> dict:
    if not pnl:
        return {"trades": 0, "win_rate": 0.0, "total_return_pct": 0.0, "max_drawdown_pct": 0.0, "profit_factor": 0.0}

    wins = [x for x in pnl if x > 0]
    losses = [x for x in pnl if x <= 0]
    total_return = sum(pnl) * 100

    # Max drawdown on cumulative PnL curve
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for r in pnl:
        equity += r
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else 99.0

    return {
        "trades": len(pnl),
        "win_rate": round(len(wins) / len(pnl) * 100, 1),
        "total_return_pct": round(total_return, 2),
        "max_drawdown_pct": round(max_dd * 100, 2),
        "profit_factor": profit_factor,
    }


def _strategy_sma_crossover(
    close: pd.Series,
    sma20: pd.Series,
    sma50: pd.Series,
    adx: pd.Series,
) -> list[float]:
    """
    SMA20/50 golden/death cross filtered by ADX >= 20 (trend confirmation).
    Long only; exit on death cross or ADX collapse.
    """
    pnl: list[float] = []
    in_position = False
    entry = 0.0

    for idx in range(1, len(close)):
        p = float(close.iloc[idx])
        s20_prev = float(sma20.iloc[idx - 1]) if pd.notna(sma20.iloc[idx - 1]) else None
        s50_prev = float(sma50.iloc[idx - 1]) if pd.notna(sma50.iloc[idx - 1]) else None
        s20_curr = float(sma20.iloc[idx]) if pd.notna(sma20.iloc[idx]) else None
        s50_curr = float(sma50.iloc[idx]) if pd.notna(sma50.iloc[idx]) else None
        adx_curr = float(adx.iloc[idx]) if pd.notna(adx.iloc[idx]) else 0.0

        if None in (s20_prev, s50_prev, s20_curr, s50_curr):
            continue

        golden_cross = s20_prev <= s50_prev and s20_curr > s50_curr  # type: ignore[operator]
        death_cross = s20_prev >= s50_prev and s20_curr < s50_curr  # type: ignore[operator]

        if not in_position and golden_cross and adx_curr >= 20:
            in_position = True
            entry = p
        elif in_position and (death_cross or adx_curr < 14):
            pnl.append((p - entry) / entry)
            in_position = False

    if in_position:
        pnl.append((float(close.iloc[-1]) - entry) / entry)

    return pnl


def _strategy_bb_rsi(
    close: pd.Series,
    rsi: pd.Series,
    bb_lower: pd.Series,
    bb_upper: pd.Series,
) -> list[float]:
    """
    Mean-reversion: buy when price < BB-lower AND RSI < 35; exit when price > BB-mid OR RSI > 60.
    """
    bb_mid = (bb_upper + bb_lower) / 2
    pnl: list[float] = []
    in_position = False
    entry = 0.0

    for idx in range(1, len(close)):
        p = float(close.iloc[idx])
        r = float(rsi.iloc[idx - 1]) if pd.notna(rsi.iloc[idx - 1]) else 50.0
        bl = float(bb_lower.iloc[idx - 1]) if pd.notna(bb_lower.iloc[idx - 1]) else p
        bu = float(bb_upper.iloc[idx - 1]) if pd.notna(bb_upper.iloc[idx - 1]) else p
        bm = float(bb_mid.iloc[idx - 1]) if pd.notna(bb_mid.iloc[idx - 1]) else p

        if not in_position and p < bl and r < 35:
            in_position = True
            entry = p
        elif in_position and (p > bm or r > 60 or p > bu):
            pnl.append((p - entry) / entry)
            in_position = False

    if in_position:
        pnl.append((float(close.iloc[-1]) - entry) / entry)

    return pnl


def multi_strategy_backtest(
    close: pd.Series,
    rsi: pd.Series,
    sma20: pd.Series,
    sma50: pd.Series,
    adx: pd.Series,
) -> dict:
    """
    Run three strategies and report stats for each + combined best.
    Strategies:
      1. RSI oversold (legacy)
      2. SMA20/50 golden cross + ADX filter
      3. Bollinger Band + RSI mean-reversion
    """
    if close is None or len(close) < 100:
        return {
            "best_strategy": "rsi_oversold",
            "strategies": {},
            "trades": 0,
            "win_rate": 0.0,
            "total_return_pct": 0.0,
            "max_drawdown_pct": 0.0,
            "profit_factor": 0.0,
        }

    bb_std = close.rolling(20).std()
    bb_upper = sma20 + 2 * bb_std
    bb_lower = sma20 - 2 * bb_std

    pnl_rsi = []
    in_pos = False
    entry = 0.0
    for idx in range(1, len(close)):
        p = float(close.iloc[idx])
        r = float(rsi.iloc[idx - 1]) if pd.notna(rsi.iloc[idx - 1]) else 50.0
        if not in_pos and r < 30:
            in_pos, entry = True, p
        elif in_pos and r > 55:
            pnl_rsi.append((p - entry) / entry)
            in_pos = False
    if in_pos:
        pnl_rsi.append((float(close.iloc[-1]) - entry) / entry)

    pnl_sma = _strategy_sma_crossover(close, sma20, sma50, adx)
    pnl_bb = _strategy_bb_rsi(close, rsi, bb_lower, bb_upper)

    stats = {
        "rsi_oversold": _pnl_stats(pnl_rsi),
        "sma_crossover": _pnl_stats(pnl_sma),
        "bb_mean_reversion": _pnl_stats(pnl_bb),
    }

    # Pick best strategy by total return (with at least 3 trades)
    best_name = "rsi_oversold"
    best_return = stats["rsi_oversold"]["total_return_pct"]
    for name, s in stats.items():
        if s["trades"] >= 3 and s["total_return_pct"] > best_return:
            best_return = s["total_return_pct"]
            best_name = name

    best = stats[best_name]
    return {
        "best_strategy": best_name,
        "strategies": stats,
        "trades": best["trades"],
        "win_rate": best["win_rate"],
        "total_return_pct": best["total_return_pct"],
        "max_drawdown_pct": best["max_drawdown_pct"],
        "profit_factor": best["profit_factor"],
    }
