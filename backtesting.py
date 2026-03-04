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
