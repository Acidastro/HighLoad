"""
Генерация графиков latency и throughput из CSV отчётов Locust.
"""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

REPORTS_DIR = Path(__file__).parent.parent / "reports"
CHARTS_DIR = Path(__file__).parent.parent / "reports" / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

LEVELS = [1, 10, 100, 1000]
PHASES = ["before_index", "after_index"]
PHASE_LABELS = {"before_index": "До индекса", "after_index": "После индекса"}
LEVEL_FILES = {1: "L1", 10: "L2", 100: "L3", 1000: "L4"}

COLORS = {
    "50%": "#2196F3",   # синий
    "95%": "#FF9800",   # оранжевый
    "99%": "#F44336",   # красный
}


def load_history(phase: str, level: int) -> pd.DataFrame:
    path = REPORTS_DIR / phase / f"{LEVEL_FILES[level]}_stats_history.csv"
    df = pd.read_csv(path)
    # Нормализуем время — от нуля
    df["Timestamp"] = df["Timestamp"] - df["Timestamp"].min()
    return df


def plot_latency_phase(phase: str) -> None:
    """Один график latency для одной фазы — все уровни нагрузки."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f"Latency {PHASE_LABELS[phase]}", fontsize=14, fontweight="bold")

    for idx, level in enumerate(LEVELS):
        ax = axes[idx // 2][idx % 2]
        try:
            df = load_history(phase, level)
            # Aggregated строки (тип запроса = "Aggregated")
            agg = df[df["Name"] == "Aggregated"]
            for pct, color in COLORS.items():
                if pct in agg.columns:
                    ax.plot(agg["Timestamp"], agg[pct], label=f"p{pct.replace('%', '')}", color=color)
        except Exception as e:
            ax.text(0.5, 0.5, f"Нет данных: {e}", ha="center", va="center")
        ax.set_title(f"{level} users")
        ax.set_xlabel("Время (сек)")
        ax.set_ylabel("Latency (мс)")
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = CHARTS_DIR / f"latency_{phase}.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  Сохранён: {out}")


def plot_throughput_phase(phase: str) -> None:
    """Один график throughput для одной фазы — все уровни нагрузки."""
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(f"Throughput (RPS) {PHASE_LABELS[phase]}", fontsize=14, fontweight="bold")

    for idx, level in enumerate(LEVELS):
        ax = axes[idx // 2][idx % 2]
        try:
            df = load_history(phase, level)
            agg = df[df["Name"] == "Aggregated"]
            if "Requests/s" in agg.columns:
                ax.plot(agg["Timestamp"], agg["Requests/s"], color="#4CAF50", label="RPS")
        except Exception as e:
            ax.text(0.5, 0.5, f"Нет данных: {e}", ha="center", va="center")
        ax.set_title(f"{level} users")
        ax.set_xlabel("Время (сек)")
        ax.set_ylabel("Requests/sec")
        ax.legend()
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    out = CHARTS_DIR / f"throughput_{phase}.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  Сохранён: {out}")


def plot_comparison() -> None:
    """Сравнительный график p95 latency до и после индекса по уровням."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Сравнение: до и после индекса", fontsize=14, fontweight="bold")

    p95_before, p95_after = [], []
    rps_before, rps_after = [], []

    for level in LEVELS:
        for phase, p95_list, rps_list in [
            ("before_index", p95_before, rps_before),
            ("after_index", p95_after, rps_after),
        ]:
            try:
                path = REPORTS_DIR / phase / f"{LEVEL_FILES[level]}_stats.csv"
                df = pd.read_csv(path)
                agg = df[df["Name"] == "Aggregated"]
                if not agg.empty:
                    p95_list.append(float(agg["95%"].values[0]))
                    rps_list.append(float(agg["Requests/s"].values[0]))
                else:
                    p95_list.append(0)
                    rps_list.append(0)
            except Exception:
                p95_list.append(0)
                rps_list.append(0)

    x = range(len(LEVELS))
    w = 0.35
    ax1.bar([i - w / 2 for i in x], p95_before, w, label="До индекса", color="#F44336")
    ax1.bar([i + w / 2 for i in x], p95_after, w, label="После индекса", color="#4CAF50")
    ax1.set_title("p95 Latency (мс)")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels([str(l) + " users" for l in LEVELS])
    ax1.set_ylabel("мс")
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis="y")

    ax2.bar([i - w / 2 for i in x], rps_before, w, label="До индекса", color="#F44336")
    ax2.bar([i + w / 2 for i in x], rps_after, w, label="После индекса", color="#4CAF50")
    ax2.set_title("Throughput (RPS)")
    ax2.set_xticks(list(x))
    ax2.set_xticklabels([str(l) + " users" for l in LEVELS])
    ax2.set_ylabel("req/s")
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    out = CHARTS_DIR / "comparison.png"
    plt.savefig(out, dpi=120)
    plt.close()
    print(f"  Сохранён: {out}")


if __name__ == "__main__":
    print("Генерация графиков...")
    for phase in PHASES:
        print(f"\nФаза: {PHASE_LABELS[phase]}")
        plot_latency_phase(phase)
        plot_throughput_phase(phase)
    print("\nСравнительный график:")
    plot_comparison()
    print("\nГотово!")
