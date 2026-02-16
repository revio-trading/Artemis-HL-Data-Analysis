"""
Analysis & Visualization of comparison_output.json
Buckets mismatches by percentage, shows worst offenders, and plots charts.
"""

import json
from collections import defaultdict
from datetime import datetime, timezone

INPUT_FILE = "comparison_output.json"

# ─── Mismatch buckets ────────────────────────────────────────────────────────
BUCKETS = [
    ("OK (< 0.5%)",       0,    0.5),
    ("0.5% – 1%",         0.5,  1),
    ("1% – 5%",           1,    5),
    ("5% – 10%",          5,    10),
    ("10% – 25%",         10,   25),
    ("25% – 50%",         25,   50),
    ("50% – 100%",        50,   100),
    ("100% – 250%",       100,  250),
    ("250% – 500%",       250,  500),
    ("> 500%",            500,  float("inf")),
]


def load_data(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def analyse(data: dict):
    # ── Collect every data-point that has both sources ────────────────────
    all_points: list[dict] = []          # {address, date, art, hl, pct, abs}
    bucket_counts = defaultdict(int)
    missing_count = 0
    per_address_mismatches: dict[str, list] = defaultdict(list)

    for addr_block in data["addresses"]:
        addr = addr_block["address"]
        for day in addr_block["series"]:
            pct = day["diff"]["pct"]
            match = day["diff"]["match"]

            if pct is None:
                missing_count += 1
                continue

            point = {
                "address": addr,
                "date": day["date"],
                "artemis": day["artemis"]["value"],
                "hyperliquid": day["hyperliquid"]["value"],
                "art_ts": day["artemis"]["last_timestamp"],
                "hl_ts": day["hyperliquid"]["last_timestamp"],
                "pct": pct,
                "abs": day["diff"]["abs"],
                "match": match,
            }
            all_points.append(point)

            for label, lo, hi in BUCKETS:
                if lo <= pct < hi:
                    bucket_counts[label] += 1
                    break

            if not match:
                per_address_mismatches[addr].append(point)

    total_compared = len(all_points)

    # ── Print bucket table ───────────────────────────────────────────────
    print("=" * 65)
    print("MISMATCH DISTRIBUTION")
    print(f"  Total compared pairs : {total_compared:,}")
    print(f"  Missing (one side)   : {missing_count:,}")
    print("=" * 65)
    print(f"{'Bucket':<20} {'Count':>8} {'%':>8}  Bar")
    print("-" * 65)
    for label, _, _ in BUCKETS:
        c = bucket_counts.get(label, 0)
        pct_of_total = c / total_compared * 100 if total_compared else 0
        bar = "█" * int(pct_of_total / 2)
        print(f"{label:<20} {c:>8,} {pct_of_total:>7.1f}%  {bar}")
    print("-" * 65)

    # ── Worst single-day mismatches ──────────────────────────────────────
    worst = sorted(all_points, key=lambda p: p["pct"], reverse=True)[:20]
    print("\nTOP 20 WORST SINGLE-DAY MISMATCHES")
    print(f"{'Address':<14} {'Date':<12} {'Artemis':>14} {'Hyperliquid':>14} {'Diff%':>8}")
    print("-" * 65)
    for p in worst:
        print(
            f"{p['address'][:12]}… {p['date']:<12} "
            f"{p['artemis']:>14,.0f} {p['hyperliquid']:>14,.0f} "
            f"{p['pct']:>7.1f}%"
        )

    # ── Addresses with most mismatch days ────────────────────────────────
    addr_mismatch_counts = {
        a: len(pts) for a, pts in per_address_mismatches.items()
    }
    worst_addrs = sorted(addr_mismatch_counts.items(), key=lambda x: -x[1])[:20]
    print(f"\nTOP 20 ADDRESSES BY MISMATCH DAY COUNT")
    print(f"{'Address':<44} {'Mismatch days':>14} {'Avg pct%':>10}")
    print("-" * 70)
    for addr, cnt in worst_addrs:
        avg_pct = sum(p["pct"] for p in per_address_mismatches[addr]) / cnt
        print(f"{addr:<44} {cnt:>14} {avg_pct:>9.1f}%")

    return all_points, bucket_counts, per_address_mismatches, missing_count


def visualize(all_points, bucket_counts, per_address_mismatches):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.ticker as mticker
    except ImportError:
        print("\n⚠  matplotlib not installed – skipping charts. pip install matplotlib")
        return

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle("Artemis vs Hyperliquid — Perp Account Value Comparison", fontsize=14, y=0.98)

    # ── 1. Bucket bar chart ──────────────────────────────────────────────
    ax = axes[0, 0]
    labels = [b[0] for b in BUCKETS]
    counts = [bucket_counts.get(l, 0) for l in labels]
    colors = [
        "#2ecc71",   # OK
        "#f1c40f",   # 0.5-1
        "#e67e22",   # 1-5
        "#e74c3c",   # 5-10
        "#c0392b",   # 10-25
        "#8e44ad",   # 25-50
        "#6c3483",   # 50-100
        "#1a5276",   # 100-250
        "#0b5345",   # 250-500
        "#17202a",   # >500
    ]
    bars = ax.barh(labels[::-1], counts[::-1], color=colors[::-1])
    ax.set_xlabel("Number of address-day pairs")
    ax.set_title("Distribution of Diff %")
    for bar, c in zip(bars, counts[::-1]):
        if c > 0:
            ax.text(bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2,
                    f"{c:,}", va="center", fontsize=9)

    # ── 2. Histogram of pct diffs (log scale, capped at 500%) ────────────
    ax = axes[0, 1]
    pcts = [p["pct"] for p in all_points if p["pct"] is not None]
    ax.hist(pcts, bins=100, color="#3498db", edgecolor="white", linewidth=0.3)
    ax.set_xlabel("Diff %")
    ax.set_ylabel("Count")
    ax.set_title("Histogram of Diff % (all pairs)")
    ax.set_yscale("log")
    ax.axvline(0.5, color="green", linestyle="--", linewidth=1, label="0.5% threshold")
    ax.legend()

    # ── 3. Scatter: Artemis vs Hyperliquid values ────────────────────────
    ax = axes[1, 0]
    art_vals = [p["artemis"] for p in all_points]
    hl_vals = [p["hyperliquid"] for p in all_points]
    scatter_colors = ["#2ecc71" if p["match"] else "#e74c3c" for p in all_points]
    ax.scatter(hl_vals, art_vals, c=scatter_colors, alpha=0.15, s=8, linewidths=0)
    # Perfect-match line
    lo = min(min(art_vals), min(hl_vals))
    hi = max(max(art_vals), max(hl_vals))
    ax.plot([lo, hi], [lo, hi], "k--", linewidth=0.8, alpha=0.5)
    ax.set_xlabel("Hyperliquid value ($)")
    ax.set_ylabel("Artemis value ($)")
    ax.set_title("Artemis vs Hyperliquid (green=OK, red=mismatch)")
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"${x/1e6:.1f}M"))

    # ── 4. Mismatch days per address (top 30) ────────────────────────────
    ax = axes[1, 1]
    addr_counts = sorted(
        ((a, len(pts)) for a, pts in per_address_mismatches.items()),
        key=lambda x: -x[1],
    )[:30]
    addrs_short = [a[:8] + "…" for a, _ in addr_counts]
    cnts = [c for _, c in addr_counts]
    ax.barh(addrs_short[::-1], cnts[::-1], color="#e74c3c")
    ax.set_xlabel("Mismatch days")
    ax.set_title("Top 30 addresses by mismatch count")

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    out = "analysis_charts_normalized.png"
    plt.savefig(out, dpi=150)
    print(f"\n📊 Charts saved to {out}")
    plt.close()


def main():
    data = load_data(INPUT_FILE)
    print(f"Loaded {INPUT_FILE}  (generated {data['generated_at']})\n")

    all_points, bucket_counts, per_addr, missing = analyse(data)
    visualize(all_points, bucket_counts, per_addr)
    print("\nDone.")


if __name__ == "__main__":
    main()
