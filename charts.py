from pathlib import Path

import matplotlib.pyplot as plt


def build_price_chart(history, ticker: str) -> str | None:
    if history is None or history.empty:
        return None

    out_dir = Path("charts")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{ticker.replace('/', '_')}.png"

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(history.index, history["Close"], label="Close", linewidth=1.7)
    if "SMA20" in history.columns:
        ax.plot(history.index, history["SMA20"], label="SMA20", linewidth=1.0)
    if "SMA50" in history.columns:
        ax.plot(history.index, history["SMA50"], label="SMA50", linewidth=1.0)
    ax.set_title(f"{ticker} price chart")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)
    return str(out_path)
